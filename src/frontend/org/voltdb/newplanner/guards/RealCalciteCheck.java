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

package org.voltdb.newplanner.guards;

import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.newplanner.SqlTask;
import org.voltdb.planner.PlanningErrorException;

/**
 * A check that always fail.
 * @author Yiqun Zhang
 * @since 8.4
 */
public class RealCalciteCheck extends CalciteCheck {

    @Override
    protected boolean doCheck(String sql) {
        try {
            return new SqlTask(sql).isDDL();
        } catch (SqlParseException e) {
            throw new PlanningErrorException(e.getMessage());
        }
    }

    @Override
    protected boolean isNegativeCheck() {
        return false;
    }
}
