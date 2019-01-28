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

package org.voltdb.plannerv2.converter;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.plannerv2.ColumnTypes;

public class TypeConverter {

    public static void setType(AbstractExpression ae, RelDataType rdt) {
        VoltType vt = ColumnTypes.getVoltType(rdt.getSqlTypeName());
        Preconditions.checkNotNull(vt);
        setType(ae, vt, rdt.getPrecision());
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
