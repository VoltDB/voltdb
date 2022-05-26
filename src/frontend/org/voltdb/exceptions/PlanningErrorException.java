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

import java.util.Formatter;

public class PlanningErrorException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public PlanningErrorException(String msg) {
        super(msg);
    }

    /**
     * Formatted error msg, the same way we use String.format.
     * @param format format string
     * @param args args in the format string
     */
    public PlanningErrorException(String format, Object... args) {
        this(new Formatter().format(format, args).toString());
    }
    public PlanningErrorException(Throwable e) {
        super(e);
    }
    public PlanningErrorException(String msg, Throwable e) {
       super(msg, e);
    }

    /**
     * Create an exception with stacktrace erased. Used for reporting
     * parsing/validation errors without the long backtrace list, to mimick
     * user-observable error message from VoltCompiler.VoltCompilerException.
     * @param msg exception message
     * @param ignored not used
     */
    public PlanningErrorException(String msg, int ignored) {
        super(msg, null, true, false);
    }
}
