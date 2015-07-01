/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.compiler;

public class CodeBlockCompilerException extends RuntimeException {

    private static final long serialVersionUID = -7394428315910573491L;

    public CodeBlockCompilerException() {
    }

    public CodeBlockCompilerException(String message, Throwable cause) {
        super(message, cause);
    }

    public CodeBlockCompilerException(String message) {
        super(message);
    }

    public CodeBlockCompilerException(Throwable cause) {
        super(cause);
    }

}
