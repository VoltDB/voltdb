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

#pragma once

#include <vector>
#include <cstdio>
#include <string>

namespace voltdb {

class StackTrace {
public:
    StackTrace();
    ~StackTrace();

    static void printMangledAndUnmangledToFile(FILE *targetFile);

    static void printStackTrace() {
        StackTrace st;
        for (int ii=1; ii < st.m_traces.size(); ii++) {
            printf("   %s\n", st.m_traces[ii].c_str());
        }
    }

    static std::string stringStackTrace();

    void printLocalTrace() {
        for (int ii=1; ii < m_traces.size(); ii++) {
            printf("   %s\n", m_traces[ii].c_str());
        }
    }

private:
    char** m_traceSymbols;
    std::vector<std::string> m_traces;
};

}
