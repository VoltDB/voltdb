/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
package org.voltdb.messaging;

public enum RecoveryMessageType {
    /*
     * Message containing freshly scanned tuples to be inserted
     */
    ScanTuples,
    /*
     * Message indicating that the table scan is complete, future polling
     * will produce delta data
     */
    ScanComplete,
    /*
     * Message containing whole tuples that are either updates or inserts
     */
    MergeTuples,
    /*
     * Message containing primary keys that must be deleted
     */
    DeletePkeys,
    /*
     * Generated when all recovery data for a table has been generated
     */
    Complete,
    /*
     * Not used in the EE. Sites receiving blocks of data ack them with this message
     */
    Ack,
    /*
     * Not used in the EE. A recovering partition sends a message of this type
     * to the source partition when its priority queue is synced. It contains
     * the last committed txnId when the recovering partition heard from all initiators.
     * The source partition sends back a response with the same subject
     * indicating the last committed txnid at the source partition before sending recovery data.
     * The recovering partition will then start running procedures after the txnid it receives
     * from the source partition.
     */
    Initiate;
}
