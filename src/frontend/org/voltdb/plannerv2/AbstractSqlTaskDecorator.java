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

package org.voltdb.plannerv2;

import org.apache.calcite.sql.SqlNode;

/**
 * Extend this class to create a decorator for a {@link SqlTask}.
 * @author Yiqun Zhang
 * @since 8.4
 */
public abstract class AbstractSqlTaskDecorator implements SqlTask {

    final SqlTask m_taskToDecorate;

    public AbstractSqlTaskDecorator(SqlTask taskToDecorate) {
        m_taskToDecorate = taskToDecorate;
    }

    @Override
    public boolean isDDL() {
        return m_taskToDecorate.isDDL();
    }

    @Override
    public boolean isDML() {
        return m_taskToDecorate.isDML();
    }

    @Override
    public boolean isDQL() {
        return m_taskToDecorate.isDQL();
    }

    @Override
    public String getSQL() {
        return m_taskToDecorate.getSQL();
    }

    @Override
    public SqlNode getParsedQuery() {
        return m_taskToDecorate.getParsedQuery();
    }
}
