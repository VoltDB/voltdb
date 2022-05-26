/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
#ifndef IOSFLAGSAVER_H
#define IOSFLAGSAVER_H

#include <ostream>

namespace voltdb {
/**
 * This is a class to save the state of the
 * flags of an ostream.  One uses it this way:
 *   Here the ios flags of someStream have some state
 *   value, say the base is decimal.
 *   {
 *      IosFlagSaver saver(someStream);
 *      // Print something in hex.
 *      someStream << hex << val;
 *   }
 *   ... Here someStream will have the original
 *   state values, printing in decimal.
 *   cf. https://stackoverflow.com/questions/2273330/restore-the-state-of-stdcout-after-manipulating-it.
 */
class IOSFlagSaver {
public:
    explicit IOSFlagSaver(std::ostream& _ios):
        ios(_ios),
        f(_ios.flags()) {
    }
    ~IOSFlagSaver() {
        ios.flags(f);
    }

    IOSFlagSaver(const IOSFlagSaver &rhs) = delete;
    IOSFlagSaver& operator= (const IOSFlagSaver& rhs) = delete;

private:
    std::ostream& ios;
    std::ios::fmtflags f;
};
}

#endif /* not(defined(IOSFLAGSAVER_H)) */
