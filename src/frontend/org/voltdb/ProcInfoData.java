/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

/**
 * Instantiate-able class with the same values as ProcInfo.
 * This is used to override the values for a procedure by
 * passing in a set of ProcInfo.Data object with a set of
 * procedure names to the compiler.
 *
 * @see ProcInfo
 */
public class ProcInfoData {
    /** See ProcInfo.partitionInfo() */
    public String partitionInfo = "";
    /** See ProcInfo.singlePartition() */
    public boolean singlePartition = false;
}
