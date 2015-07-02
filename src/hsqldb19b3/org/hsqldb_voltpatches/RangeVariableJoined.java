/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.HsqlNameManager.SimpleName;
import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.RangeVariable.RangeIteratorMain;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HashSet;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.map.ValuePool;

/**
 * Metadata for range joined variables
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.2.9
 * @since 1.9.0
 */
public class RangeVariableJoined extends RangeVariable {

    RangeVariable[] rangeArray;

    public RangeVariableJoined(Table table, SimpleName alias,
                               OrderedHashSet columnList,
                               SimpleName[] columnNameList,
                               CompileContext compileContext) {

        super(table, alias, columnList, columnNameList, compileContext);

        setParameters();
    }

    private void setParameters() {

        QuerySpecification qs =
            (QuerySpecification) this.rangeTable.getQueryExpression();

        this.rangeArray = qs.rangeVariables;

        for (int i = 0; i < rangeArray.length; i++) {
            if (rangeArray[i].isLeftJoin) {
                hasLeftJoin = true;
            }

            if (rangeArray[i].isRightJoin) {
                hasRightJoin = true;
            }

            if (rangeArray[i].isLateral) {
                hasLateral = true;
            }

            break;
        }
    }

    public RangeVariable[] getBaseRangeVariables() {
        return rangeArray;
    }

    public void setRangeTableVariables() {
        super.setRangeTableVariables();
    }

    public void setJoinType(boolean isLeft, boolean isRight) {
        super.setJoinType(isLeft, isRight);
    }

    public void addNamedJoinColumns(OrderedHashSet columns) {
        super.addNamedJoinColumns(columns);
    }

    public void addColumn(int columnIndex) {
        super.addColumn(columnIndex);
    }

    public void addAllColumns() {
        super.addAllColumns();
    }

    public void addNamedJoinColumnExpression(String name, Expression e) {
        super.addNamedJoinColumnExpression(name, e);
    }

    public ExpressionColumn getColumnExpression(String name) {

        ExpressionColumn col = super.getColumnExpression(name);

        if (col == null) {
            col = rangeArray[0].getColumnExpression(name);
        }

        return col;
    }

    public Table getTable() {
        return super.getTable();
    }

    public boolean hasSingleIndexCondition() {
        return super.hasSingleIndexCondition();
    }

    public boolean setDistinctColumnsOnIndex(int[] colMap) {
        return super.setDistinctColumnsOnIndex(colMap);
    }

    /**
     * Used for sort
     */
    public Index getSortIndex() {
        return super.getSortIndex();
    }

    /**
     * Used for sort
     */
    public boolean setSortIndex(Index index, boolean reversed) {
        return super.setSortIndex(index, reversed);
    }

    public boolean reverseOrder() {
        return super.reverseOrder();
    }

    public OrderedHashSet getColumnNames() {
        return super.getColumnNames();
    }

    public OrderedHashSet getUniqueColumnNameSet() {
        return super.getUniqueColumnNameSet();
    }

    public int findColumn(String schemaName, String tableName,
                          String columnName) {

        if (tableAlias != null) {
            return super.findColumn(schemaName, tableName, columnName);
        }

        boolean hasNamed = rangeArray[0].namedJoinColumnExpressions != null;
        int     count    = 0;

        if (hasNamed) {
            count = rangeArray[0].namedJoinColumnExpressions.size();

            if (rangeArray[0].namedJoinColumnExpressions.containsKey(
                    columnName)) {
                if (tableName != null) {
                    return -1;
                }

                return super.findColumn(schemaName, tableName, columnName);
            }
        }

        for (int i = 0; i < rangeArray.length; i++) {
            RangeVariable currentRange = rangeArray[i];
            int colIndex = currentRange.findColumn(schemaName, tableName,
                                                   columnName);

            if (colIndex > -1) {
                if (!hasNamed) {
                    return count + colIndex;
                }

                for (int j = 0; j < colIndex; j++) {
                    ColumnSchema col = currentRange.rangeTable.getColumn(j);

                    if (!currentRange.namedJoinColumnExpressions.containsKey(
                            col.getNameString())) {
                        count++;
                    }
                }

                return count;
            }

            count += currentRange.rangeTable.getColumnCount();

            if (hasNamed) {
                count -= currentRange.namedJoinColumnExpressions.size();
            }
        }

        return -1;
    }

    public SimpleName getColumnAlias(int i) {
        return super.getColumnAlias(i);
    }

    public boolean hasColumnAlias() {
        return super.hasColumnAlias();
    }

    public SimpleName getTableAlias() {
        return super.getTableAlias();
    }

    public RangeVariable getRangeForTableName(String name) {

        if (tableAlias != null) {
            return super.getRangeForTableName(name);
        }

        for (int i = 0; i < rangeArray.length; i++) {
            RangeVariable range = rangeArray[i].getRangeForTableName(name);

            if (range != null) {
                return range;
            }
        }

        return null;
    }

    /**
     * Add all columns to a list of expressions
     */
    public void addTableColumns(HsqlArrayList exprList) {
        super.addTableColumns(exprList);
    }

    /**
     * Add all columns to a list of expressions
     */
    public int addTableColumns(HsqlArrayList exprList, int position,
                               HashSet exclude) {
        return super.addTableColumns(exprList, position, exclude);
    }

    public void addTableColumns(RangeVariable subRange, Expression expression,
                                HashSet exclude) {

        int index = getFirstColumnIndex(subRange);

        addTableColumns(expression, index,
                        subRange.rangeTable.getColumnCount(), exclude);
    }

    protected int getFirstColumnIndex(RangeVariable subRange) {

        if (subRange == this) {
            return 0;
        }

        int count = 0;

        for (int i = 0; i < rangeArray.length; i++) {
            int index = rangeArray[i].getFirstColumnIndex(subRange);

            if (index == -1) {
                count += rangeArray[i].rangeTable.getColumnCount();
            } else {
                return count + index;
            }
        }

        return -1;
    }

    /**
     * Removes reference to Index to avoid possible memory leaks after alter
     * table or drop index
     */
    public void setForCheckConstraint() {
        super.setForCheckConstraint();
    }

    /**
     * used before condition processing
     */
    public Expression getJoinCondition() {
        return super.getJoinCondition();
    }

    public void addJoinCondition(Expression e) {
        super.addJoinCondition(e);
    }

    public void resetConditions() {
        super.resetConditions();
    }

    public void replaceColumnReference(RangeVariable range,
                                       Expression[] list) {}

    public void replaceRangeVariables(RangeVariable[] ranges,
                                      RangeVariable[] newRanges) {
        super.replaceRangeVariables(ranges, newRanges);
    }

    public void resolveRangeTable(Session session, RangeGroup rangeGroup,
                                  RangeGroup[] rangeGroups) {
        super.resolveRangeTable(session, rangeGroup, rangeGroups);
    }

    /**
     * Retreives a String representation of this obejct. <p>
     *
     * The returned String describes this object's table, alias
     * access mode, index, join mode, Start, End and And conditions.
     *
     * @return a String representation of this object
     */
    public String describe(Session session, int blanks) {

        RangeVariableConditions[] conditionsArray = joinConditions;
        StringBuffer              sb;
        String b = ValuePool.spaceString.substring(0, blanks);

        sb = new StringBuffer();

        String temp = "INNER";

        if (isLeftJoin) {
            temp = "LEFT OUTER";

            if (isRightJoin) {
                temp = "FULL";
            }
        } else if (isRightJoin) {
            temp = "RIGHT OUTER";
        }

        sb.append(b).append("join type=").append(temp).append("\n");
        sb.append(b).append("table=").append(rangeTable.getName().name).append(
            "\n");

        if (tableAlias != null) {
            sb.append(b).append("alias=").append(tableAlias.name).append("\n");
        }

        boolean fullScan = !conditionsArray[0].hasIndexCondition();

        sb.append(b).append("access=").append(fullScan ? "FULL SCAN"
                                                       : "INDEX PRED").append(
                                                       "\n");

        for (int i = 0; i < conditionsArray.length; i++) {
            RangeVariableConditions conditions = this.joinConditions[i];

            if (i > 0) {
                sb.append(b).append("OR condition = [");
            } else {
                sb.append(b).append("condition = [");
            }

            sb.append(conditions.describe(session, blanks + 2));
            sb.append(b).append("]\n");
        }

        return sb.toString();
    }

    public RangeIteratorMain getIterator(Session session) {
        return super.getIterator(session);
    }
}
