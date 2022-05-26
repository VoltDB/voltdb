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

package org.voltdb;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.voltdb.client.ClientResponse;

/**
 * Sysproc variant of VoltNTProcedure adds the all-hosts method.
 *
 */
public class VoltNTSystemProcedure extends VoltNonTransactionalProcedure {

    /**
     * Run a non-transactional (only) procedure on each live host.
     */
    protected CompletableFuture<Map<Integer,ClientResponse>> callNTProcedureOnAllHosts(String procName, Object... params) {
        return m_runner.callAllNodeNTProcedure(procName, params);
    }

    protected String getHostname() {
        return m_runner.getHostname();
    }

    protected boolean isAdminConnection() {
        return m_runner.isAdminConnection();
    }

    protected long getClientHandle() {
        return m_runner.getClientHandle();
    }

    protected String getUsername() {
        return m_runner.getUsername();
    }

    protected boolean isRestoring() {
        return m_runner.isRestoring();
    }

    protected void noteRestoreCompleted() {
        m_runner.noteRestoreCompleted();
    }

    public InetSocketAddress getRemoteAddress() {
        return m_runner.getRemoteAddress();
    }

    protected ProcedureRunnerNT getProcedureRunner() {
        return m_runner;
    }
}
