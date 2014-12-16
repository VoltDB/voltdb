/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

    // deterministically seeded PRNG
    Random rand = new Random(0);

    public UniqueIdGenerator(int appCount) {
        this.appCount = appCount;
        counts = new long[appCount];
        nextSubCounts = new int[appCount];
    }

    long[] getNextAppIdAndUniqueDeviceId() {
        int appId = Math.min((int) Math.abs(rand.nextGaussian() * appCount / 3), appCount - 1);

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

        return new long[] { appId, value };
    }

    long expectedCountForApp(int appId) {
        return counts[appId];
    }
}
