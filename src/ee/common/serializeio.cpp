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
#include "common/serializeio.h"
#include "common/executorcontext.hpp"

using namespace voltdb;

void FallbackSerializeOutput::expand(size_t minimum_desired) {
    /*
     * Leave some space for message headers and such, almost 50 megabytes
     */
    size_t maxAllocationSize = ((1024 * 1024 *50) - (1024 * 32));
    if (fallbackBuffer_ != NULL || minimum_desired > maxAllocationSize) {
        if (fallbackBuffer_ != NULL) {
            char *temp = fallbackBuffer_;
            fallbackBuffer_ = NULL;
            delete []temp;
        }
        throw SQLException(SQLException::volt_output_buffer_overflow,
            "Output from SQL stmt overflowed output/network buffer of 50mb (-32k for message headers). "
            "Try a \"limit\" clause or a stronger predicate.");
    }
    fallbackBuffer_ = new char[maxAllocationSize];
    ::memcpy(fallbackBuffer_, data(), position_);
    setPosition(position_);
    initialize(fallbackBuffer_, maxAllocationSize);
    ExecutorContext::getExecutorContext()->getTopend()->fallbackToEEAllocatedBuffer(fallbackBuffer_, maxAllocationSize);
}


