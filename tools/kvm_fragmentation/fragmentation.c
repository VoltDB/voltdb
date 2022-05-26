/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef BUFFER_SIZE
#define BUFFER_SIZE 2097152
#endif
#ifndef MIN_BUFFER_FILL
#define MIN_BUFFER_FILL 4096
#endif
#ifndef MAX_BUFFER_FILL
#define MAX_BUFFER_FILL 8192
#endif
#ifndef MAX_LIVE_BUFFERS
#define MAX_LIVE_BUFFERS 5000
#endif
#ifndef PRELOAD_CHUNKS
#define PRELOAD_CHUNKS 2000
#endif
#ifndef PRELOAD_CHUNK_SIZE
#define PRELOAD_CHUNK_SIZE 65536
#endif
#ifndef PERIODIC_CHUNK_MIN_SIZE
#define PERIODIC_CHUNK_MIN_SIZE 40
#endif
#ifndef PERIODIC_CHUNK_MAX_SIZE
#define PERIODIC_CHUNK_MAX_SIZE 200
#endif
#ifndef PERIODIC_CHUNK_LIFESPAN_MICROS
#define PERIODIC_CHUNK_LIFESPAN_MICROS 100
#endif
#ifndef APPROX_RUNTIME
#define APPROX_RUNTIME 600
#endif
#ifndef APPROX_STATS_SECONDS
#define APPROX_STATS_SECONDS 5
#endif

int main() {
    printf("Approximate configured runtime is %d seconds.\n", APPROX_RUNTIME);
    const int LOOP_COUNT = APPROX_RUNTIME * 1000 / 2 / 5;
    const int BUFFER_FILL_RANGE = MAX_BUFFER_FILL - MIN_BUFFER_FILL;
    const int PERIODIC_CHUNK_RANGE =
            PERIODIC_CHUNK_MAX_SIZE - PERIODIC_CHUNK_MIN_SIZE;
    int i, sz, idx, allocatedBuffers = 0;
    void* buffers[MAX_LIVE_BUFFERS];
    void* preloadChunks[PRELOAD_CHUNKS];
    memset(buffers, 0, sizeof(buffers));

    printf("Starting test...\n");
    for (i = 0; i < LOOP_COUNT; i++) {
        if (i < PRELOAD_CHUNKS) {
            preloadChunks[i] = malloc(PRELOAD_CHUNK_SIZE);
            memset(preloadChunks[i], 0, PRELOAD_CHUNK_SIZE);
        } else if (i == PRELOAD_CHUNKS) {
            printf("Finished preloading long-lived chunks.\n");
        }
        if (i % 4 == 0 && allocatedBuffers > 0) {
            idx = rand() % MAX_LIVE_BUFFERS;
            if (buffers[idx]) {
                free(buffers[idx]);
                buffers[idx] = NULL;
                allocatedBuffers--;
            }
        }
        idx = rand() % MAX_LIVE_BUFFERS;
        if (buffers[idx]) {
            free(buffers[idx]);
        } else {
            allocatedBuffers++;
        }
        sz = MIN_BUFFER_FILL + (rand() % BUFFER_FILL_RANGE);
        buffers[idx] = malloc(sz > BUFFER_SIZE ? sz : BUFFER_SIZE);
        memset(buffers[idx], 0, sz);
        if (i % (APPROX_STATS_SECONDS * 200) == 0) {
            printf("Progress - %f, fill ratio - %f\n",
                    (double)i/(double)LOOP_COUNT,
                    (double)allocatedBuffers/(double)MAX_LIVE_BUFFERS);
        }
        usleep(5000);
    }

    printf("Draining remaining buffers...\n");
    for (i = 0; i < MAX_LIVE_BUFFERS; i++) {
        if (buffers[i]) {
            free(buffers[i]);
        }
    }

    printf("Starting quiesced period...\n");
    for (i = 0; i < LOOP_COUNT; i++) {
        sz = PERIODIC_CHUNK_MIN_SIZE + (rand() % PERIODIC_CHUNK_RANGE);
        void* periodicChunk = malloc(sz);
        memset(periodicChunk, 0, sz);
        usleep(PERIODIC_CHUNK_LIFESPAN_MICROS);
        free(periodicChunk);
        usleep(5000 - PERIODIC_CHUNK_LIFESPAN_MICROS);
        if (i % (APPROX_STATS_SECONDS * 200) == 0) {
            printf("Progress - %f\n", (double)i/(double)LOOP_COUNT);
        }
    }

    printf("Freeing long-lived chunks...\n");
    for (i = 0; i < PRELOAD_CHUNKS; i++) {
        free(preloadChunks[i]);
    }

    printf("Test finished.\n");
    exit(0);
}
