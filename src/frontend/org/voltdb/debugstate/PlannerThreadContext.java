/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.debugstate;

import java.io.Serializable;

import org.voltdb.compiler.AsyncCompilerResult;
import org.voltdb.compiler.AsyncCompilerWork;

public class PlannerThreadContext extends VoltThreadContext implements Serializable, Comparable<PlannerThreadContext> {
    private static final long serialVersionUID = -5981541715574116664L;

    public int siteId;
    public AsyncCompilerWork[] compilerWork = null;
    public AsyncCompilerResult[] compilerResults = null;

    @Override
    public int compareTo(PlannerThreadContext o) {
        if (o == null) return -1;
        return siteId - o.siteId;
    }
}
