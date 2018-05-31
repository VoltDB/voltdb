/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

public interface ClientInterfaceRepairCallback {
    public void repairCompleted(int partitionId, long initiatorHSId);

    /**
     * Callback invoked when managed leadership migration occurs. There should
     * be no transaction to repair in this case.
     * @param partitionId   The partition ID
     * @param initiatorHSId The new leader's HSID
     */
    default void leaderMigrated(int partitionId, long initiatorHSId) {}
}
