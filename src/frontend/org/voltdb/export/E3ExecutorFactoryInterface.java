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

package org.voltdb.export;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public interface E3ExecutorFactoryInterface {
    /**
     * Get an executor for an {@link E3ExportDataSource} identified by partitionId and tableName
     *
     * @param partitionId
     * @return {@link ListeningExecutorService} allocated
     */
    public ListeningExecutorService getExecutor(int partitionId);

    /**
     * Free an executor used by an export data source identified by partitionId and tableName
     *
     * @param partitionId
     */
    public void freeExecutor(int partitionId);

    /**
     * Shutdown all the executors
     */
    public void shutdown();
}
