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

package org.voltdb.utils;

/**
 * This exception is thrown to indicate the feature that the user
 * is attempting to use isn't in place yet (if it ever will be).
 *
 * Ideally, you'll see less and less of this exception.
 *
 */
public class NotImplementedException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public NotImplementedException(String message) {
        super(message);
    }
}
