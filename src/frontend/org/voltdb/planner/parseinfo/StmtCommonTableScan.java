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
package org.voltdb.planner.parseinfo;

import java.util.List;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;

public class StmtCommonTableScan extends StmtTableScan {

    private m_isReplicated = false;

    public StmtCommonTableScan(String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
    }
    @Override
    public String getTableName() {
        return m_tableAlias;
    }

    @Override
    public boolean getIsReplicated() {
        return m_isReplicated;
    }

    @Override
    public List<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScansOrCommonTables;
    }

    @Override
    public String getColumnName(int columnIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public AbstractExpression processTVE(TupleValueExpression expr, String columnName) {
        // TODO Auto-generated method stub
        return null;
    }

}
