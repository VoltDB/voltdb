/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.rules.physical;

public final class Constants {
    private Constants() {
    }

    static final int JOIN_SPLIT_COUNT = 1;
    static final int VALUES_SPLIT_COUNT = 1;
    // TODO: why 30?
    static public final int DISTRIBUTED_SPLIT_COUNT = 30;

    // TODO: verify this
    static public final int MAX_TABLE_ROW_COUNT = 1000000;
}
