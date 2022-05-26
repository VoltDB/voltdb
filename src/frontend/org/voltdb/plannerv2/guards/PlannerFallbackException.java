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

package org.voltdb.plannerv2.guards;

/**
 * An exception indicates that we should go back and use the legacy
 * VoltDB parser and planner.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class PlannerFallbackException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PlannerFallbackException(String message) {
        super(message);
    }

    public PlannerFallbackException(Throwable cause) {
        super(cause);
    }
}
