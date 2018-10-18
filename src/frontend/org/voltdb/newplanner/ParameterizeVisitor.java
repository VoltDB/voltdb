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

package org.voltdb.newplanner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.voltcore.utils.Pair;

/**
 * The visitor that can replace all the {@link SqlLiteral} in the query to {@link SqlDynamicParam}.
 * It is used for parameterizing a query.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class ParameterizeVisitor extends SqlBasicVisitor<SqlNode> {

    private final List<SqlLiteral> m_sqlLiteralList = new ArrayList<>();
    private int m_dynamicParamIndex = 0;

    /**
     * Get the list of parameter values.
     * @return the list of parameter values.
     */
    public List<SqlLiteral> getSqlLiteralList() {
        return m_sqlLiteralList;
    }

    @Override
    public SqlNode visit(SqlLiteral literal) {
        /*
         * For SqlLiteral, we replace it with a SqlDynamicParam.
         * There is a special case in SqlWindow.
         * SqlWindow is for the OVER clause, for example: OVER (PARTITION BY cnt ORDER BY name).
         * This kind of node does not have a chance to be parameterized,
         * but its "isRows" flag's type is SqlLiteral, which will cause an error if
         * we try to parameterize it.
         */
        if (literal.getTypeName() == SqlTypeName.BOOLEAN
                || literal.getTypeName() == SqlTypeName.SYMBOL) {
            return null;
        }
        m_sqlLiteralList.add(literal);
        return new SqlDynamicParam(m_dynamicParamIndex++, literal.getParserPosition());
    }

    @Override
    public SqlNode visit(SqlDynamicParam param) {
        throw new RuntimeException("Shouldn't be parameterizing a query that already has parameters.");
    }

    @Override
    public SqlNode visit(SqlCall call) {
        // The parameters should be indexed in the order they appeared in the query.
        // So we need to sort the operands by their parser positions here.
        // We pair the operands with their original indexes before sorting, so those
        // original indexes can still be retrieved later for operand update.
        List<Pair<Integer, SqlNode>> indexedOperands = toIndexedPairList(call.getOperandList());
        // Sort the operands based on their positions. We will ignore the equal cases
        // because the operands of the same parent node won't overlap.
        indexedOperands.sort(PositionBasedIndexedSqlNodePairComparator.INSTANCE);

        // Visit the operands in the order of their parser positions.
        for (Pair<Integer, SqlNode> indexedOperand : indexedOperands) {
            SqlNode operand = indexedOperand.getSecond();
            SqlNode convertedOperand = operand.accept(this);
            if (operand instanceof SqlLiteral
                    && convertedOperand != null) {
                call.setOperand(indexedOperand.getFirst(), convertedOperand);
            }
        }
        return null;
    }

    @Override
    public SqlNode visit(SqlNodeList nodeList) {
        // This function is very similar to SqlNode visit(SqlCall call), see comments there.
        // An example use case of the SqlNodeList is the IN clause: select * from t where a in (1, ?, 2);
        List<Pair<Integer, SqlNode>> indexedNodes = toIndexedPairList(nodeList.getList());
        indexedNodes.sort(PositionBasedIndexedSqlNodePairComparator.INSTANCE);
        for (Pair<Integer, SqlNode> indexedNode : indexedNodes) {
            SqlNode node = indexedNode.getSecond();
            SqlNode convertedNode = node.accept(this);
            if (node instanceof SqlLiteral) {
                nodeList.set(indexedNode.getFirst(), convertedNode);
            }
        }
        return null;
    }

    private <T> List<Pair<Integer, T>> toIndexedPairList(List<T> originalList) {
        List<Pair<Integer, T>> retval = new ArrayList<>();
        int pos = 0;
        for (T elem : originalList) {
            if (elem != null) {
                retval.add(new Pair<>(pos, elem));
            }
            ++pos;
        }
        return retval;
    }

    private static final class PositionBasedIndexedSqlNodePairComparator implements Comparator<Pair<Integer,SqlNode>> {
        final static PositionBasedIndexedSqlNodePairComparator INSTANCE =
                new PositionBasedIndexedSqlNodePairComparator();
        @Override
        public int compare(Pair<Integer,SqlNode> o1, Pair<Integer,SqlNode> o2) {
            SqlParserPos lPos = o1.getSecond().getParserPosition();
            SqlParserPos rPos = o2.getSecond().getParserPosition();
            return lPos.startsBefore(rPos) ? -1 : 1;
        }
    }
}
