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

#include "FragmentManager.h"

namespace voltdb {

    /**
     * Order by length first to avoid expensive comparisons, then by content
     */
    bool operator< (const CachedPlan &x, const CachedPlan &y) {
        if (x.length < y.length) return true;
        if (x.length > y.length) return false;
        int cmp = memcmp(x.core->plan, y.core->plan, std::min(x.length, y.length));
        return cmp < 0;
    }

}
