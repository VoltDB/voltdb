/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter;

import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.FunctionExpression;

public class RexConverterHelper {

    public static AbstractExpression createFunctionExpression(
            RelDataType relDataType,
            String funcName,
            int funcId,
            List<AbstractExpression> operands,
            String impliedArg) {
        FunctionExpression fe = new FunctionExpression();
        fe.setAttributes(funcName, impliedArg, funcId);
        fe.setArgs(operands);
        TypeConverter.setType(fe, relDataType);
        return fe;
    }

    public static AbstractExpression createFunctionExpression(
            RelDataType relDataType,
            String funcName,
            int funcId,
            List<AbstractExpression> operands) {
        return createFunctionExpression(relDataType, funcName, funcId, operands, null);
    }

}
