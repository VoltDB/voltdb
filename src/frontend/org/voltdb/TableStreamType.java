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
package org.voltdb;

/*
 * Define the different modes of operation for table streams.
 *
 * IMPORTANT: Keep this enum in sync with the C++ equivalent in types.h!
 *            Values are serialized through JNI and IPC as integers.
 */
public enum TableStreamType {
    /*
     * A snapshot stream of a table is a copy of all the tuple data contained
     * in table at the time that the stream was created. The snapshot
     * is maintained using a copy on write mechanism.
     */
    SNAPSHOT,
    /*
     * An elastic index build stream initiates the capture and ongoing maintenance
     * of a tuple index that supports elastic re-balancing for topology changes.
     */
    ELASTIC_INDEX,
    /*
     * An elastic index read stream materializes the index created by ELASTIC_INDEX
     * using the SNAPSHOT streaming mechanism.
     */
    ELASTIC_INDEX_READ,
    /*
     * Activation clears the index and the referenced tuples.
     */
    ELASTIC_INDEX_CLEAR,
    /*
     * A stream of tuple data that can be used to retrieve the latest state of a table
     * that is actively being modified. The stream starts by transporting all the tuple data
     * and then transports the set of modified and deleted tuples in a separate synchronous phase.
     */
    RECOVERY
}
