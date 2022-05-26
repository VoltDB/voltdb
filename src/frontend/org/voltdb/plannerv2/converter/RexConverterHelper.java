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

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.FunctionSQL;
import org.voltdb.VoltType;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.InComparisonExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.VectorValueExpression;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QuantifierType;

import java.util.ArrayList;
import java.util.List;

public class RexConverterHelper {

    public static AbstractExpression createFunctionExpression(
            RelDataType relDataType, String funcName, List<AbstractExpression> operands, String impliedArg, String optionalArgs) {
        int functionId = FunctionSQL.voltGetFunctionId(funcName);
        if (functionId == FunctionSQL.FUNC_VOLT_INVALID) {
            functionId = FunctionForVoltDB.getFunctionID(funcName);
        }
        if (functionId == FunctionSQL.FUNC_VOLT_INVALID) {
            throw new CalcitePlanningException("Unsupported function:" + funcName);
        }
        return createFunctionExpression(relDataType, funcName, functionId, operands, impliedArg, optionalArgs);
    }

    public static AbstractExpression createFunctionExpression(
            RelDataType relDataType, String funcName, int funcId, List<AbstractExpression> operands, String impliedArg, String optionalArgs) {
        if (funcId == FunctionSQL.FUNC_VOLT_INVALID) {
            return createFunctionExpression(relDataType, funcName, operands, impliedArg, optionalArgs);
        }
        FunctionExpression fe = new FunctionExpression();
        fe.setAttributes(funcName, impliedArg, optionalArgs, funcId);
        fe.setArgs(operands);
        RexConverter.setType(fe, relDataType);
        return fe;
    }

    public static AbstractExpression createFunctionExpression(
            VoltType voltType, String funcName, int funcId, List<AbstractExpression> operands,
            String impliedArg, String optionalArgs) {
        FunctionExpression fe = new FunctionExpression();
        fe.setAttributes(funcName, impliedArg, optionalArgs, funcId);
        fe.setArgs(operands);
        RexConverter.setType(fe, voltType, voltType.getMaxLengthInBytes());
        return fe;
    }

    public static AbstractExpression createToTimestampFunctionExpression(
            RelDataType relDataType, ExpressionType intervalOperatorType, List<AbstractExpression> aeOperands) {
        // There must be two operands
        Preconditions.checkArgument(2 == aeOperands.size());
        // One of them is timestamp and another one is interval (BIGINT) in microseconds
        AbstractExpression timestamp = null;
        AbstractExpression interval = null;
        if (aeOperands.get(0).getValueType() == VoltType.TIMESTAMP) {
            timestamp = aeOperands.get(0);
        } else if (aeOperands.get(0).getValueType() == VoltType.BIGINT) {
            interval = aeOperands.get(0);
        }
        if (aeOperands.get(1).getValueType() == VoltType.TIMESTAMP) {
            timestamp = aeOperands.get(1);
        } else if (aeOperands.get(1).getValueType() == VoltType.BIGINT) {
            interval = aeOperands.get(1);
        }
        if (timestamp == null || interval == null) {
            throw new CalcitePlanningException("Invalid arguments for VoltDB TO_TIMESTAMP function");
        }
        RexConverter.setType(timestamp, VoltType.TIMESTAMP, VoltType.TIMESTAMP.getLengthInBytesForFixedTypes());
        RexConverter.setType(interval, VoltType.BIGINT, VoltType.BIGINT.getLengthInBytesForFixedTypes());

        // SINCE_EPOCH
        List<AbstractExpression> epochOperands = new ArrayList<>();
        epochOperands.add(timestamp);
        String impliedArgMicrosecond = "MICROSECOND";
        AbstractExpression sinceEpochExpr = createFunctionExpression(
                VoltType.BIGINT, "since_epoch", FunctionSQL.voltGetSinceEpochId(impliedArgMicrosecond),
                epochOperands, impliedArgMicrosecond, null);

        // Plus/Minus interval
        AbstractExpression plusExpr = new OperatorExpression(intervalOperatorType, sinceEpochExpr, interval);
        RexConverter.setType(plusExpr, VoltType.BIGINT, VoltType.BIGINT.getLengthInBytesForFixedTypes());

        // TO_TIMESTAMP
        List<AbstractExpression> timestampOperands = new ArrayList<>();
        timestampOperands.add(plusExpr);
        AbstractExpression timestampExpr = createFunctionExpression(
                relDataType, "to_timestamp", FunctionSQL.voltGetToTimestampId(impliedArgMicrosecond),
                timestampOperands, impliedArgMicrosecond, null);
        RexConverter.setType(timestampExpr, relDataType);

        return timestampExpr;
    }

    public static AbstractExpression createInComparisonExpression(
            RelDataType relDataType, List<AbstractExpression> aeOperands) {
        Preconditions.checkArgument(aeOperands.size() > 0);
        // The left expression should be the same for all operands because it is IN expression
        AbstractExpression leftInExpr = aeOperands.get(0).getLeft();
        Preconditions.checkNotNull(leftInExpr);
        AbstractExpression rightInExpr = new VectorValueExpression();
        List<AbstractExpression> inArgs = new ArrayList<>();
        for (AbstractExpression expr : aeOperands) {
            Preconditions.checkNotNull(expr.getRight());
            inArgs.add(expr.getRight());
        }
        rightInExpr.setArgs(inArgs);

        ComparisonExpression inExpr = new InComparisonExpression();
        inExpr.setLeft(leftInExpr);
        inExpr.setRight(rightInExpr);
        inExpr.setQuantifier(QuantifierType.ANY);
        inExpr.finalizeValueTypes();
        RexConverter.setType(inExpr, relDataType);
        return inExpr;
    }
}
