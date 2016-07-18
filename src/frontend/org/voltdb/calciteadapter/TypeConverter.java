package org.voltdb.calciteadapter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;

public class TypeConverter {

    private static String sqlTypeNameToVoltTypeName(SqlTypeName typeName) {
        if (typeName.getName().equals("VARCHAR")) {
            return "STRING";
        }
        else if (typeName.getName().equals("DOUBLE")) {
            return "FLOAT";
        }

        return typeName.getName();
    }



    public static void setType(AbstractExpression ae, RelDataType rdt) {
        VoltType vt = VoltType.valueOf(sqlTypeNameToVoltTypeName(rdt.getSqlTypeName()));
        ae.setValueType(vt);

        if (vt.isVariableLength()) {
            ae.setValueSize(rdt.getPrecision());
        }
    }
}
