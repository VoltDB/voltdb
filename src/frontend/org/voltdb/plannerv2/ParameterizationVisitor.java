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
 * The visitor that can replace all {@link SqlLiteral} in the query to
 * {@link SqlDynamicParam}. It is used for parameterizing a query.
 * After parameterization, the original parameter values are preserved in the
 * {@link ParameterizedSqlTask}.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class ParameterizationVisitor extends SqlBasicVisitor<SqlNode> {

    private final List<SqlLiteral> m_sqlLiteralList = new ArrayList<>();
    private int m_dynamicParamIndex = 0;

    /**
     * @return the list of parameter values.
     */
    public List<SqlLiteral> getSqlLiteralList() {
        return m_sqlLiteralList;
    }

    @Override public SqlNode visit(SqlLiteral literal) {
        /*
         * For SqlLiteral, we replace it with a SqlDynamicParam.
         * There are special cases where some classes have SqlLiteral to
         * store internal states:
         * * SqlWindow.isRows, allowPartial - BOOLEAN SqlLiteral
         * * SqlExplain.detailLevel, depth, format are SYMBOL SqlLiterals.
         *   The explain command in Calcite is different. I ticketed it as ENG-14838.
         * * SqlJoin.natrual - BOOLEAN, joinType - SYMBOL, conditionType - SYMBOL
         * * SqlMatchRecognize.strictStart, strictEnd - BOOLEAN, rowsPerMatch - SYMBOL
         *     SqlMatchRecognize.interval - SqlIntervalQualifier.typeName()
         *     This involves many interval types, which makes guarding them in
         *     parameterization really hard. The good side is that this is not
         *     something that we currently support in VoltDB.
         *     A ticket (ENG-14839) is created to note this.
         *
         * In summary, we will need to ignore BOOLEAN and SYMBOL types in
         * parameterization. This place is noted in ENG-6840 for adding support of
         * BOOLEAN type into VoltDB. We need to invent a new strategy then.
         */
        if (literal.getTypeName() == SqlTypeName.BOOLEAN
                || literal.getTypeName() == SqlTypeName.SYMBOL) {
            // Return null, nothing will change.
            return null;
        }
        m_sqlLiteralList.add(literal);
        return new SqlDynamicParam(m_dynamicParamIndex++, literal.getParserPosition());
    }

    @Override public SqlNode visit(SqlDynamicParam param) {
        throw new RuntimeException("Shouldn't be parameterizing a query that already has parameters.");
    }

    @Override public SqlNode visit(SqlCall call) {
        // The parameters should be indexed in the order they appeared in the query.
        // So we need to sort the operands by their parser positions here.
        // We pair the operands with their original indexes before sorting, so those
        // original indexes can still be retrieved later for operand update.
        List<Pair<Integer, SqlNode>> indexedOperands = toIndexedPairList(call.getOperandList());
        // Sort the operands based on their positions. We will ignore the equal cases
        // because the operands of the same parent node won't overlap.
        indexedOperands.sort(PositionBasedIndexedSqlNodePairComparator.INSTANCE);

        // Visit the operands in the order of their positions in the query.
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

    @Override public SqlNode visit(SqlNodeList nodeList) {
        // This function is very similar to SqlNode visit(SqlCall call), see comments there.
        // An example use case of the SqlNodeList is the IN clause: select * from t where a in (1, ?, 2);
        List<Pair<Integer, SqlNode>> indexedNodes = toIndexedPairList(nodeList.getList());
        indexedNodes.sort(PositionBasedIndexedSqlNodePairComparator.INSTANCE);
        for (Pair<Integer, SqlNode> indexedNode : indexedNodes) {
            SqlNode node = indexedNode.getSecond();
            SqlNode convertedNode = node.accept(this);
            if (node instanceof SqlLiteral
                    && convertedNode != null) {
                nodeList.set(indexedNode.getFirst(), convertedNode);
            }
        }
        return null;
    }

    /**
     * Pair the list elements with their ordinal indexes, then put the pairs into
     * another list.
     *
     * @param originalList the list of elements.
     * @return the generated list of pairs.
     */
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

    /**
     * Comparator for Pair<Integer, SqlNode>, based on the SqlNode's parser position.
     *
     * @author Yiqun Zhang
     * @since 9.0
     */
    private static final class PositionBasedIndexedSqlNodePairComparator implements Comparator<Pair<Integer,SqlNode>> {
        final static PositionBasedIndexedSqlNodePairComparator INSTANCE =
                new PositionBasedIndexedSqlNodePairComparator();

        @Override public int compare(Pair<Integer,SqlNode> o1, Pair<Integer,SqlNode> o2) {
            SqlParserPos lPos = o1.getSecond().getParserPosition();
            SqlParserPos rPos = o2.getSecond().getParserPosition();
            return lPos.startsBefore(rPos) ? -1 : 1;
        }
    }
}
