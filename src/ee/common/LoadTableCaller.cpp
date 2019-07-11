/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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

#include "common/LoadTableCaller.h"

namespace voltdb {

// Initialize all of the valid callers
const LoadTableCaller LoadTableCaller::s_callers[] = {
    LoadTableCaller(SNAPSHOT_REPORT_UNIQ_VIOLATIONS, true, false, true),
    LoadTableCaller(SNAPSHOT_THROW_ON_UNIQ_VIOLATION, false, false, true),
    LoadTableCaller(DR, true, false, true),
    LoadTableCaller(BALANCE_PARTITIONS, true, false, true),
    LoadTableCaller(CLIENT, false, true, false),
    LoadTableCaller(INTERNAL, false, false, true)
};

}
