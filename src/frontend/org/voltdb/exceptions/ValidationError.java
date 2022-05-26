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

package org.voltdb.exceptions;

/**
 * Validation error includes invariant violations when validating a plan node, e.g.
 * in all AbstractPlanNode validate methods, and all AbstractExpression validate methods.
 */
public class ValidationError extends PlanningErrorException {
    public ValidationError(String msg) {
        super(msg);
    }
    public ValidationError(Throwable e) {
        super(e);
    }
    public ValidationError(String msg, Throwable e) {
        super("ValidationError: " + msg, e);
    }
    public ValidationError(String format, Object... args) {
        super("ValidationError: " + format, args);
    }
    // Hides PlanningErrorException ctor with stack trace erased,
    // because we always need the stack trace to understand what is going on.

    @Override
    public String toString() {
        return "ValidationError: " + super.toString();
    }
}
