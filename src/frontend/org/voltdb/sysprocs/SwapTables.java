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

package org.voltdb.sysprocs;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

/**
 * Non-transactional procedure that turns around and creates a SQL statement
 * string "@SwapTables TableA TableB" and passes it on to AdHoc.
 *
 */
public class SwapTables extends AdHocNTBase {

    public CompletableFuture<ClientResponse> run(String theTable, String otherTable) {
        String sql = "@SwapTables " + theTable + " " + otherTable;
        Object[] userParams = null;

        CatalogContext context = VoltDB.instance().getCatalogContext();
        List<String> sqlStatements = new ArrayList<>(1);
        sqlStatements.add(sql);

        return runNonDDLAdHoc(context,
                sqlStatements,
                true, // infer partitioning...?
                null, // no partition key
                ExplainMode.NONE,
                false, // not a large query
                true,  // IS swap tables
                userParams);
    }
}
