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

package org.voltdb;

/**
 * A volt-specific exception to be thrown when some flavor of unresolvable
 * type issue occurs (mismatched type, bad cast, cast leading to over/underflow,
 * etc.).  We'll probably eventually want things more specialized than this to
 * signal various SQL-mandated error conditions(?) but this hopefully is at
 * least incrementally better than throwing generic java exceptions in such
 * cases.
 */
// Derive this from RuntimeException for now so we don't have to go back
// and add checked exception handling all everywhere /lazy
public class VoltTypeException extends RuntimeException
{
    private static final long serialVersionUID = -7774100755422441459L;

    public VoltTypeException()
    {
    }

    public VoltTypeException(String message)
    {
        super(message);
    }

    public VoltTypeException(Throwable cause)
    {
        super(cause);
    }

    public VoltTypeException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
