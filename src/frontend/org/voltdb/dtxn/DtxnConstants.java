/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.dtxn;

public abstract class DtxnConstants {

    /** If the txn id is this, do a dump from the ExecSite */
    public static final long DUMP_REQUEST_TXNID = -101;

    /** Dtxn requires 1 dependency response per partition */
    public static final int MULTIPARTITION_DEPENDENCY = 0x40000000;

    public static final long DUMMY_LAST_SEEN_TXN_ID = -1;

}
