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

#ifdef USEJNI
#ifndef JAVASETUP_H_
#define JAVASETUP_H_

#include <cassert>
#include <cstdlib>
#include "jni.h"
using namespace std;

extern JNIEnv *env;
extern JavaVM *jvm;

void loadJVM() {
    JavaVMOption options[10];
    JavaVMInitArgs vm_args;
    long status = 0;

    int n = 0;
    string classpath("-Djava.class.path=tasks:");
    options[n++].optionString = strdup(classpath.c_str());
    //cout << "CLASSPATH: " << options[0].optionString << endl;
    memset(&vm_args, 0, sizeof(vm_args));
    vm_args.version = JNI_VERSION_1_6;
    vm_args.nOptions = n;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = false;

    void *envptr;
    status = JNI_CreateJavaVM(&jvm, &envptr, &vm_args);
    if (status == JNI_ERR) {
        fprintf(stderr, "Error creating VM\n");
        exit(1);
    }
    env = reinterpret_cast<JNIEnv*>(envptr);
    cout << "status: " << status << endl; cout.flush();
    assert(env);
    assert(jvm);

    cout << "version: " << env->GetVersion() << endl; cout.flush();
}

void setupJVMForThread(int threadId) {
    if (threadId == 0)
        return;

    int res = jvm->AttachCurrentThread((void**)&env, NULL);
    printf("res was: %d\n", res);
    assert(res >= 0);
}

#endif // JAVASETUP_H_
#endif // USEJNI
