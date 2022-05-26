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

#pragma once

#include <vector>
#include <cstdio>
#include <string>
#include <iostream>
#include <sstream>

namespace voltdb {

class StackTrace {
public:
    // By default do not include the frame with the constructor
    StackTrace(uint32_t skipFrames = 1);
    ~StackTrace();

    static void printMangledAndUnmangledToFile(FILE *targetFile);

    static void printStackTrace() {
        StackTrace(2).printLocalTrace();
    }

    static std::string stringStackTrace(std::string prefix) {
        std::ostringstream stacked;
        StackTrace(2).streamLocalTrace(stacked, prefix);
        return stacked.str();
    }

    static void streamStackTrace(std::ostream& stream, std::string prefix) {
        StackTrace(2).streamLocalTrace(stream, prefix);
    }

    void printLocalTrace() {
        streamLocalTrace(std::cout, "    ");
    }

    void streamLocalTrace(std::ostream& stream, std::string prefix) {
        for (int ii=0; ii < m_traces.size(); ii++) {
            stream << prefix << m_traces[ii] << '\n';
        }
        stream.flush();
    }

private:
    char** m_traceSymbols;
    std::vector<std::string> m_traces;
};

}
