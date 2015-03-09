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

#include "common/MiscUtil.h"

namespace voltdb
{

std::vector<std::string> MiscUtil::splitString(const std::string &str, char delimiter)
{
    std::vector<std::string> vec;
    size_t begin = 0;
    while (true) {
        size_t end = str.find(delimiter, begin);
        if (end == std::string::npos) {
            if (begin != str.size()) {
                vec.push_back(str.substr(begin));
            }
            break;
        }
        vec.push_back(str.substr(begin, end - begin));
        begin = end + 1;
    }
    return vec;
}

std::vector<std::string> MiscUtil::splitToTwoString(const std::string &str, char delimiter)
{
    std::vector<std::string> vec;
    size_t end = str.find(delimiter);
    if (end == std::string::npos) {
        vec.push_back(str);
    } else {
        vec.push_back(str.substr(0, end));
        vec.push_back(str.substr(end + 1));
    }
    return vec;
}

} // namespace voltdb
