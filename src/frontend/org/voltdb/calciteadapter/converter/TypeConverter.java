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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;

import com.google_voltpatches.common.collect.ImmutableMap;

public class TypeConverter {

    private final static ImmutableMap<String, VoltType> calciteToVoltDBTypeMap;

    private final static ImmutableMap<VoltType, BasicSqlType> voltToCalciteTypeMap;

    private static ImmutableMap<String, VoltType> mapCalciteToVoltDB() {
        ImmutableMap.Builder<String, VoltType> mapBuilder = ImmutableMap.builder();

        for (SqlTypeName calciteType : SqlTypeName.values()) {
            switch(calciteType.getName()) {
                // NUMERIC
                case "BOOLEAN"      : mapBuilder.put("BOOLEAN", VoltType.BOOLEAN);
                    break;
                case "TINYINT"      : mapBuilder.put("TINYINT", VoltType.TINYINT);
                    break;
                case "SMALLINT"     : mapBuilder.put("SMALLINT", VoltType.SMALLINT);
                    break;
                case "INTEGER"      : mapBuilder.put("INTEGER", VoltType.INTEGER);
                    break;
                case "BIGINT"       : mapBuilder.put("BIGINT", VoltType.BIGINT);
                    break;
                case "DECIMAL"      : mapBuilder.put("DECIMAL", VoltType.DECIMAL);
                    break;
                case "REAL"         : mapBuilder.put("REAL", VoltType.FLOAT);
                    break;
                case "DOUBLE"       : mapBuilder.put("DOUBLE", VoltType.FLOAT);
                    break;
                case "FLOAT"        : mapBuilder.put("FLOAT", VoltType.FLOAT);
                    break;

                // DATE TIME
                case "TIMESTAMP"    : mapBuilder.put("TIMESTAMP", VoltType.TIMESTAMP);
                    break;
                case "INTERVAL_DAY" : mapBuilder.put("INTERVAL_DAY", VoltType.BIGINT);
                    break;

                // STRING
                case "CHAR"         : mapBuilder.put("CHAR", VoltType.STRING);
                    break;
                case "VARCHAR"      : mapBuilder.put("VARCHAR", VoltType.STRING);
                    break;

                // VARBINARY
                case "VARBINARY"    : mapBuilder.put("VARBINARY", VoltType.VARBINARY);
                    break;

                // DEFAULT INVALID
                default             : mapBuilder.put(calciteType.getName(), VoltType.INVALID);
            }
        }
        return mapBuilder.build();
    }

    private static ImmutableMap<VoltType, BasicSqlType> mapVoltDBToCalcite() {
        ImmutableMap.Builder<VoltType, BasicSqlType> mapBuilder = ImmutableMap.builder();

        for (VoltType voltType : VoltType.values()) {
            switch(voltType) {
                // NUMERIC
                case BOOLEAN      : mapBuilder.put(VoltType.BOOLEAN,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BOOLEAN));
                    break;
                case TINYINT      : mapBuilder.put(VoltType.TINYINT,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TINYINT));
                    break;
                case SMALLINT     : mapBuilder.put(VoltType.SMALLINT,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.SMALLINT));
                    break;
                case INTEGER      : mapBuilder.put(VoltType.INTEGER,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER));
                    break;
                case BIGINT       : mapBuilder.put(VoltType.BIGINT,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT));
                    break;
                case DECIMAL      : mapBuilder.put(VoltType.DECIMAL,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL));
                    break;
                case FLOAT         : mapBuilder.put(VoltType.FLOAT,
                        // Calcite DOUBLE, REAL, FLOAT all map to VoltDB FLOAT
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT));
                    break;

                // DATE TIME
                case TIMESTAMP    : mapBuilder.put(VoltType.TIMESTAMP,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP));
                    break;
                // VoltDB maps INTERVAL_DAY to BIGINT
//                case INTERVAL_DAY : mapBuilder.put(VoltType.INTERVAL_DAY,
//                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP));
//                    break;

                // STRING
                case STRING         : mapBuilder.put(VoltType.STRING,
                        // Calcite CHAR and VARCHAR map to VoltDB STRING
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR));
                    break;

                // VARBINARY
                case VARBINARY    : mapBuilder.put(VoltType.VARBINARY,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARBINARY));
                    break;

                // DEFAULT OTHER
                default           : mapBuilder.put(voltType,
                        new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.OTHER));
                break;
            }
        }
        return mapBuilder.build();
    }

    static {
        calciteToVoltDBTypeMap = mapCalciteToVoltDB();
        voltToCalciteTypeMap = mapVoltDBToCalcite();
    }

    private static VoltType sqlTypeNameToVoltType(SqlTypeName typeName) {
        return calciteToVoltDBTypeMap.get(typeName.getName());
    }

    public static void setType(AbstractExpression ae, RelDataType rdt) {
        VoltType vt = sqlTypeNameToVoltType(rdt.getSqlTypeName());
        assert(vt != null);
        setType(ae, vt, rdt.getPrecision());
    }

    public static BasicSqlType voltTypeToSqlType(VoltType voltType) {
        BasicSqlType calciteType = voltToCalciteTypeMap.get(voltType);
        assert(calciteType != null);
        return calciteType;
    }

    public static void setType(AbstractExpression ae, VoltType vt, int precision) {

        ae.setValueType(vt);

        if (vt.isVariableLength()) {
            int size;
            if ((ae instanceof ConstantValueExpression ||
                    ae instanceof FunctionExpression)
                    &&
                    (vt != VoltType.NULL) && (vt != VoltType.NUMERIC)) {
                size = vt.getMaxLengthInBytes();
            } else {
                size = precision;
            }
            if (!(ae instanceof ParameterValueExpression)) {
                ae.setValueSize(size);
            }
        }
    }

}
