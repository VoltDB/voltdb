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

package windowing;

import java.io.IOException;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class MaxTracker implements Runnable {

    final GlobalState state;

    long previousMax = Long.MIN_VALUE;

    MaxTracker(GlobalState state) {
        this.state = state;
    }

    @Override
    public void run() {
        try {
            ClientResponse cr = state.client.callProcedure("MaxValue");
            long currentMax = cr.getResults()[0].asScalarLong();
            long previousMaxCopy = previousMax;

            synchronized(this) {
                if (currentMax == previousMax) {
                    return;
                }
                previousMax = currentMax;
            }

            // Output synchronized on global state to make this line not print in the middle
            // of other reporting lines.
            synchronized(state) {
                if (previousMaxCopy == Long.MIN_VALUE) {
                    System.out.printf("The initial maximum value for the dataset has been set to %d.\n\n",
                                      currentMax);
                }
                else {
                    System.out.printf("The maximum value for the dataset has changed from %d to %d.\n\n",
                                      previousMaxCopy, currentMax);
                }
            }
        }
        catch (IOException | ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
