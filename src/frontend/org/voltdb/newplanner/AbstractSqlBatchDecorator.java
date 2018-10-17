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

import java.util.Iterator;

/**
 * Extend this class to create a decorator for a {@link SqlBatch}.
 * @since 8.4
 * @author Yiqun Zhang
 */
public abstract class AbstractSqlBatchDecorator implements SqlBatch {

    /**
     * The {@link SqlBatch} to be decorated.
     */
    final SqlBatch m_batchToDecorate;

    public AbstractSqlBatchDecorator(SqlBatch batchToDecorate) {
        m_batchToDecorate = batchToDecorate;
    }

    @Override
    public boolean isDDLBatch() {
        return m_batchToDecorate.isDDLBatch();
    }

    @Override
    public Object[] getUserParameters() {
        return m_batchToDecorate.getUserParameters();
    }

    @Override
    public int getTaskCount() {
        return m_batchToDecorate.getTaskCount();
    }

    @Override
    public Iterator<SqlTask> iterator() {
        return m_batchToDecorate.iterator();
    }
}
