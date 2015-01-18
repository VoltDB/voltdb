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

#ifndef JAVAFIB_H_
#define JAVAFIB_H_

#include <iostream>
#include "../javasetup.h"
#include "../Task.h"
using namespace std;

class JavaFib : public Task {
public:
    JavaFib() {
        cout << "Looking for class: " << "JavaFib" << endl;
        jclass fibClassLocal = env->FindClass("JavaFib");
        if (fibClassLocal == NULL) {
            env->ExceptionDescribe();
            assert(fibClassLocal);
        }
        fibClass = static_cast<jclass>(env->NewGlobalRef(fibClassLocal));
        env->DeleteLocalRef(fibClassLocal);
        cout << "Found class\n";

        idDoOne = env->GetStaticMethodID(fibClass, "doOne", "()V");
        assert(idDoOne);
    }

    virtual void doOne() {
        env->CallStaticVoidMethod(fibClass, idDoOne);
    }

protected:
    jclass fibClass;
    jmethodID idDoOne;
};

#endif // JAVAFIB_H_
