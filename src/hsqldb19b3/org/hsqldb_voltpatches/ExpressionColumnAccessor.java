/* Copyright (c) 2001-2014, The HSQL Development Group
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

import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.Set;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of column used as assignment target.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.3.0
 * @since 2.0.0
 */
public class ExpressionColumnAccessor extends Expression {

    ColumnSchema column;

    ExpressionColumnAccessor(ColumnSchema column) {

        super(OpTypes.COLUMN);

        this.column   = column;
        this.dataType = column.getDataType();
    }

    String getAlias() {
        return column.getNameString();
    }

    void collectObjectNames(Set set) {

        set.add(column.getName());

        if (column.getName().parent != null) {
            set.add(column.getName().parent);
        }
    }

    String getColumnName() {
        return column.getNameString();
    }

    public ColumnSchema getColumn() {
        return column;
    }

    RangeVariable getRangeVariable() {
        return null;
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount, RangeGroup[] rangeGroups,
            HsqlList unresolvedSet, boolean acceptsSequences) {
        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {}

    public Object getValue(Session session) {
        return null;
    }

    public String getSQL() {
        return column.getName().statementName;
    }

    protected String describe(Session session, int blanks) {
        return column.getName().name;
    }

    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {
        return unresolvedSet;
    }

    /**
     * collects all range variables in expression tree
     */
    OrderedHashSet collectRangeVariables(RangeVariable[] rangeVariables,
                                         OrderedHashSet set) {
        return set;
    }

    Expression replaceAliasInOrderBy(Session session, Expression[] columns,
                                     int length) {
        return this;
    }

    Expression replaceColumnReferences(RangeVariable range,
                                       Expression[] list) {
        return this;
    }

    /**
     * return true if given RangeVariable is used in expression tree
     */
    boolean hasReference(RangeVariable range) {
        return false;
    }

    /**
     * SIMPLE_COLUMN expressions can be of different Expression subclass types
     */
    public boolean equals(Expression other) {

        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (opType != ((Expression) other).opType) {
            return false;
        }

        return column == ((Expression) other).getColumn();
    }

    void replaceRangeVariables(RangeVariable[] ranges,
                               RangeVariable[] newRanges) {}

    void resetColumnReferences() {}

    public boolean isIndexable(RangeVariable range) {
        return false;
    }

    public boolean isUnresolvedParam() {
        return false;
    }

    boolean isDynamicParam() {
        return false;
    }

    public Type getDataType() {
        return column.getDataType();
    }
}
