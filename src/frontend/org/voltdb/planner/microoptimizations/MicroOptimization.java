/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb.planner.microoptimizations;

import java.util.List;

import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;

public abstract class MicroOptimization {
    protected AbstractParsedStmt m_parsedStmt;

<<<<<<< HEAD
    boolean shouldRun(DeterminismMode detMode, boolean hasDeterministicStatement) { return true; }
=======
    @SuppressWarnings("static-method")
    boolean shouldRun(DeterminismMode detMode) {
        return true;
    }
>>>>>>> da5896cc756ad4082136525c0628adf4c74500e2

    public abstract List<CompiledPlan> apply(CompiledPlan plan, AbstractParsedStmt parsedStmt) ;
}
