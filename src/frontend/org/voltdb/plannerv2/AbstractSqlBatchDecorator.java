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

import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.plannerv2.guards.PlannerFallbackException;

/**
 * Extend this class to create a decorator for a {@link SqlBatch}.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public abstract class AbstractSqlBatchDecorator extends SqlBatch {

    /**
     * The {@link SqlBatch} to be decorated.
     */
    final SqlBatch m_batchToDecorate;

    /**
     * Initialize an AbstractSqlBatchDecorator.
     *
     * @param batchToDecorate the {link SqlBatch} this decorator is decorating.
     */
    public AbstractSqlBatchDecorator(SqlBatch batchToDecorate) {
        m_batchToDecorate = batchToDecorate;
    }

    @Override public CompletableFuture<ClientResponse> execute()
            throws PlanningErrorException, PlannerFallbackException {
        return m_batchToDecorate.execute();
    }

    @Override public boolean isDDLBatch() {
        return m_batchToDecorate.isDDLBatch();
    }

    @Override public Object[] getUserParameters() {
        return m_batchToDecorate.getUserParameters();
    }

    @Override public int getTaskCount() {
        return m_batchToDecorate.getTaskCount();
    }

    @Override public Iterator<SqlTask> iterator() {
        return m_batchToDecorate.iterator();
    }

    @Override Context getContext() {
        return m_batchToDecorate.getContext();
    }
}
