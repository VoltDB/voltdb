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

package org.voltdb.sysprocs;

import org.voltdb.ParameterSet;
import org.voltdb.client.ClientResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Used by Explain, ExplainJSON, ExplainView and ExplainProc as base class.
 */
abstract public class AdHocNTExplain extends AdHocNTBase {
    abstract public CompletableFuture<ClientResponse> run(String fullViewNames);

    protected String stringOf(ParameterSet params) {
        assert params.size() == 1;
        assert params.getParam(0) instanceof String;
        return (String) params.getParam(0);
    }

    @Override
    protected CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) {
        return runUsingLegacy(params);
    }
}
