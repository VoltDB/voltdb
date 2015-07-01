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

#ifndef RBTREEACCESS_H_
#define RBTREEACCESS_H_

#include <map>
#include <cstdlib>
#include "../Task.h"
using namespace std;

class RBTreeAccess : public Task {
public:
    RBTreeAccess() {
        PRIME = 1000003;
        SIZE = PRIME * 2;

        for (int i = 0; i < SIZE; i++) {
            int* val = new int;
            *val = i;
            tree[i] = val;
        }

        next = 0;
    }

    virtual void doOne() {
        int value;
        //int x = 1;
        for (int i = 0; i < 1000; i++) {
            // if (globalVar > SIZE) globalVar = 0;
            value = *(tree[next]);// + globalVar;
            next = (value + PRIME + 1) % SIZE;
            //if ((i % x) == 0) globalVar++;
        }
    }

protected:
    int PRIME;
    int SIZE;
    int next;
    map<int, int*> tree;
};

#endif // RBTREEACCESS_H_
