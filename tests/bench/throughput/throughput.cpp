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

#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include "timer.h"
#include "Workload.h"
#include "javasetup.h"
using namespace std;

#ifdef USEJNI
JNIEnv *env = NULL;
JavaVM *jvm = NULL;
#endif // USEJNI

const int MAX_PROCS = 256;
const int MAX_THREADSPERPROC = 256;

const int64_t CHUNK_TIME = 1 * 1000 * 1000;
const int64_t WARMUP_TIME = 5 * 1000 * 1000;
const int64_t COOLDOWN_TIME = 5 * 1000 * 1000;
const int64_t MEASURE_TIME = 10 * 1000 * 1000;

int numProcs;
int threadsPerProc;
string workload;

int64_t globalVar = 0;

pid_t procIds[MAX_PROCS];
pid_t localPid = 0;
pthread_t localThreads[MAX_THREADSPERPROC];

void *runLoop(void *threadIdVoid) {
    int threadId = *((int*)threadIdVoid);
    printf("Thread %d,%d started\n", localPid, threadId);

    // setup java if needed
#ifdef USEJNI
    setupJVMForThread(threadId);
#endif // USEJNI

    // wait one second to startup
    sleep(1);

    // figure out what the chunksize is for the load
    Workload load;
    load.initialize(workload, CHUNK_TIME);

    // run for 10 seconds as a warm up
    int64_t start = nowMicroSeconds();
    while ((nowMicroSeconds() - start) < WARMUP_TIME)
        load.runChunk();

    // run for 20 seconds to measure
    int64_t count = 0;
    start = nowMicroSeconds();
    while ((nowMicroSeconds() - start) < MEASURE_TIME)
        count += load.runChunk();
    int64_t duration = nowMicroSeconds() - start;

    // run for 10 seconds so other threads/procs
    // can complete measurement under load
    start = nowMicroSeconds();
    while ((nowMicroSeconds() - start) < COOLDOWN_TIME)
        load.runChunk();

    double workPerSecond = count / (duration / 1000000.0);
    printf("RESULT: %d,%d,%.2f\n", localPid, threadId, workPerSecond);

    return NULL;
}

void runAllThreads() {
    for (int i = 1; i < threadsPerProc; i++)
        pthread_create(&(localThreads[i]), NULL, runLoop, (void*)&i);

    int mainThreadId = 0;

    printf("beginning main loop.\n");
    runLoop(&mainThreadId);
    printf("main loop complete.\n");

    for (int i = 1; i < threadsPerProc; i++)
        pthread_join(localThreads[i], NULL);
}

int main(int argc, char** argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: throughput #proc #threadperproc workloadstr\n");
        return -1;
    }

    numProcs = atoi(argv[1]);
    threadsPerProc = atoi(argv[2]);
    workload = argv[3];

    //printf("In start process: %d\n", localPid);

    for (int i = 1; i < numProcs; i++) {
        if ((procIds[i] = fork()) == 0) {
            localPid = i;
            //printf("In process: %d\n", localPid);
            break;
        }
        //printf("Created process: %d\n", procIds[i]);
    }

    printf("PID: %d\n", localPid);

#ifdef USEJNI
    loadJVM();
#endif // USEJNI

    runAllThreads();

    if (localPid == 0) {
        for (int i = 1; i < numProcs; i++) {
            int status;
            wait(&status);
            assert(status == 0);
        }
    }

    return 0;
}
