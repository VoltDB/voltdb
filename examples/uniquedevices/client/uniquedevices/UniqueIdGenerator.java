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

package uniquedevices;

import java.util.Random;

public class UniqueIdGenerator {

    final int appCount;
    final long counts[];
    final int nextSubCounts[];
    final long realCardinality[];
    final int shuffleMap[];

    // deterministically seeded PRNG
    Random rand = new Random(0);

    public UniqueIdGenerator(int appCount) {
        this.appCount = appCount;
        counts = new long[appCount];
        nextSubCounts = new int[appCount];
        realCardinality = new long[appCount];
        shuffleMap = new int[appCount];

        // create a random shuffling map so the distribution of popular
        //  apps seems random in a top-N list format
        for (int i = 0; i < appCount; i++) {
            shuffleMap[i] = i;
        }
        for (int i = 0; i < appCount * 5; i++) {
            int index1 = rand.nextInt(appCount);
            int index2 = rand.nextInt(appCount);
            int temp = shuffleMap[index2];
            shuffleMap[index2] = shuffleMap[index1];
            shuffleMap[index1] = temp;
        }
    }

    long[] getNextAppIdAndUniqueDeviceId() {
        int appId = Integer.MAX_VALUE;
        while (appId >= appCount) {
            appId = Math.min((int) Math.abs(rand.nextGaussian() * appCount / 3), appCount - 1);
            appId = shuffleMap[appId];
        }

        long value = -1;

        while (value < 0) {
            int exp = 1 << nextSubCounts[appId];

            // possible match
            if ((counts[appId] % exp) == 0) {
                value = counts[appId];
            }

            // increment nextSubCounts with a wrap
            if (nextSubCounts[appId] == 30) {
                nextSubCounts[appId] = 0;
                counts[appId]++;
            }
            else {
                nextSubCounts[appId]++;
            }
        }

        if (nextSubCounts[appId] == 1) {
            realCardinality[appId]++;
        }

        return new long[] { appId, value };
    }

    long expectedCountForApp(int appId) {
        return realCardinality[appId];
    }
}
