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
package org.voltdb;

/*
 * Define two different types of ways that a table can be streams
 */
public enum TableStreamType {
    /*
     * A snapshot stream of a table is a copy of all the tuple data contained
     * in table at the time that the stream was created. The snapshot
     * is maintained using a copy on write mechanism.
     */
    SNAPSHOT,
    /*
     * A stream of tuple data that can be used to retrieve the latest state of a table
     * that is actively being modified. The stream starts by transporting all the tuple data
     * and then transports the set of modified and deleted tuples in a separate synchronous phase.
     */
    RECOVERY
}
