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

#ifndef MISC_UTIL_H_
#define MISC_UTIL_H_

#include <string>
#include <vector>

namespace voltdb
{

/**
 * Miscellaneous static utility methods.
 */
class MiscUtil
{

  public:

    /**
     * Split string on delimiter into any number of sub-strings.
     */
    static std::vector<std::string> splitString(const std::string &str, char delimiter);

    /**
     * Split string on delimiter into two sub-strings.
     */
    static std::vector<std::string> splitToTwoString(const std::string &str, char delimiter);
};

} // namespace voltdb

#endif // MISC_UTIL_H_
