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

package org.voltdb.sysprocs;

import java.util.concurrent.CompletableFuture;

import org.voltdb.VoltNTSystemProcedure;
import org.voltdb.client.ClientResponse;

public class AdHocSpForTest extends VoltNTSystemProcedure {
    public CompletableFuture<ClientResponse> run(byte[] jarfileBytes, String classesToDeleteSelector) {
        /*ParameterSet params = task.getParams();
        assert(params.size() > 1);
        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        // get the partition param which must exist
        Object[] userPartitionKey = Arrays.copyOfRange(paramArray, 1, 2);
        Object[] userParams = null;
        // There's no reason (any more) that AdHocSP's can't have '?' parameters, but
        // note that the explicit partition key argument is not considered one of them.
        if (params.size() > 2) {
            userParams = Arrays.copyOfRange(paramArray, 2, paramArray.length);
        }
        ExplainMode explainMode = isExplain ? ExplainMode.EXPLAIN_ADHOC : ExplainMode.NONE;
        dispatchAdHocCommon(task, handler, ccxn, explainMode, sql, userParams, userPartitionKey, user);*/

        return null;
    }
}
