/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.elastic;

import org.json_voltpatches.JSONObject;
import org.voltdb.CatalogContext;

public interface ElasticService {
    void shutdown();
    void updateConfig(CatalogContext context);

    /**
     * Return any metadata required to resume a running elastic operation after a cluster recovery.
     *
     * @return {@link JSONObject} with metadata required to resume the currently running elastic operation. May return
     *         {@code null}
     */
    JSONObject getResumeMetadata();
}
