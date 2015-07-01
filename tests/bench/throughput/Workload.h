/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef WORKLOAD_H_
#define WORKLOAD_H_

#include <vector>
#include <cstdio>
#include "timer.h"
#include "Task.h"
#include "registration.h"
using namespace std;

class Workload {
public:
    Workload() {
        loadSize = 0;
        load = NULL;
    }

    ~Workload() {
        if (load != NULL) {
            for (int i = 0; i < loadSize; i++)
                if (load[i] != NULL)
                    delete load[i];
            delete[] load;
        }
    }

    void initialize(string desc, int chunkDurationMicroSeconds)  {
        printf("initializing workload\n");

        // create the load from the string
        loadSize = desc.length();
        load = new Task*[loadSize];

        // this is just for safety
        for (int i = 0; i < loadSize; i++)
            load[i] == NULL;

        // this assumes the letters for each load object are known
        for (int i = 0; i < loadSize; i++) {
            char c = desc[i];
            load[i] = getTaskInstanceForLetter(c);
            assert(load[i]);
        }

        printf("determining chunksize\n");

        // determine how many runs we can do in chunkDurationMicroSeconds
        chunkSize = 1;
        int64_t end = 0, start = 0;
        while ((end - start) < chunkDurationMicroSeconds) {
            start = nowMicroSeconds();
            doN(chunkSize);
            end = nowMicroSeconds();
            chunkSize *= 2;
        }
        // we doubled one too many times
        chunkSize /= 2;

        printf("workload initialized with chunksize = %d\n", chunkSize);
    }

    void doN(int n) {
        for (int i = 0; i < n; i++)
            for (int j = 0; j < loadSize; j++)
                load[j]->doOne();
    }

    int runChunk() {
        doN(chunkSize);
        return chunkSize;
    }

protected:
    int chunkSize;
    int loadSize;
    Task **load;
};

#endif // WORKLOAD_H_
