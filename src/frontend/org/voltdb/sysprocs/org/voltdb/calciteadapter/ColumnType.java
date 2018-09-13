package org.voltdb.sysprocs.org.voltdb.calciteadapter;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltcore.utils.Pair;
import org.voltdb.VoltType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Adaptor between org.voltdb.VoltType and org.apache.calcite.sql.SqlDataTypeSpec.
 */
public class ColumnType {
    private static final Map<SqlTypeName, VoltType> s_CalciteToVoltTypeMap = new HashMap<>();
    private static final Map<VoltType, SqlTypeName> s_VoltToCalciteTypeMap = new HashMap<>();

    static {
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
            s_CalciteToVoltTypeMap.put(ct, vt);
            s_VoltToCalciteTypeMap.put(vt, ct);
        });
    }

    public static VoltType getVoltType(SqlDataTypeSpec calciteType) {
        return s_CalciteToVoltTypeMap.get(calciteType);
    }

    public static VoltType getVoltType(String calciteType) {
        switch (calciteType) {
            case "INTEGER":
                return VoltType.INTEGER;
            case "BOOLEAN":
                return VoltType.BOOLEAN;
            case "VARCHAR":
            case "VARBINARY":
                return VoltType.STRING;
            case "GEOGRAPHY":
                return VoltType.GEOGRAPHY;
            case "FLOAT":
                return VoltType.FLOAT;
            case "DECIMAL":
                return VoltType.DECIMAL;
            case "SMALLINT":
                return VoltType.SMALLINT;
            case "TINYINT":
                return VoltType.TINYINT;
            case "BIGINT":
                return VoltType.BIGINT;
            case "TIMESTAMP":
                return VoltType.TIMESTAMP;
            default:
                return VoltType.INVALID;
        }
    }

    public static SqlTypeName getCalciteType(VoltType voltType) {
        return s_VoltToCalciteTypeMap.get(voltType);
    }
}
