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

package callcenter;

import java.util.Random;

/**
 * Randomly take events from an EventSource and delay and duplicate
 * them. Uses a Zipfian distribution for delay and a fixed probablibilty
 * of duplification.
 */
public class NetworkSadnessTransformer<T> implements EventSource<T> {

    // Upstream source
    final EventSource<T> source;

    // random number generator with constant seed
    final Random rand = new Random(1);

    // uses a DelayQueue for scheduling events
    final DelayedQueue<T> delayed = new DelayedQueue<>();

    // for zipf generation
    private final int size;
    private final double skew;
    private final double bottom;

    NetworkSadnessTransformer(EventSource<T> source) {
        this.source = source;

        // zipf params and setup
        size = 1;
        skew = 1;
        double bottomtemp = 0;
        for (int i = 1; i < size; i++) {
            bottomtemp += (1 / Math.pow(i, this.skew));
        }
        bottom = bottomtemp; // because final
    }

    // Based on code by Hyunsik Choi
    // http://diveintodata.org/2009/09/zipf-distribution-generator-in-java/
    public int nextZipfDelay() {
        int value;
        double friquency = 0;
        double dice;

        value = rand.nextInt(size);
        friquency = (1.0d / Math.pow(value, this.skew)) / this.bottom;
        dice = rand.nextDouble();

        while(!(dice < friquency)) {
            value = rand.nextInt(size);
            friquency = (1.0d / Math.pow(value, this.skew)) / this.bottom;
            dice = rand.nextDouble();
        }

        return value;
    }

    /**
     * Possibly duplicate and delay by some random amount.
     */
    void transformAndQueue(T event, long systemCurrentTimeMillis) {
        // if you're super unlucky, this blows up the stack
        if (rand.nextDouble() < 0.05) {
            // duplicate this message (note recursion means maybe more than duped)
            transformAndQueue(event, systemCurrentTimeMillis);
        }

        long delayms = nextZipfDelay();
        delayed.add(systemCurrentTimeMillis + delayms, event);
    }

    /**
     * Return the next event that is safe for delivery or null
     * if there are no safe objects to deliver.
     *
     * Null response could mean no events, or could mean all events
     * are scheduled for the future.
     *
     * @param systemCurrentTimeMillis The current time.
     */
    @Override
    public T next(long systemCurrentTimeMillis) {
        // drain all the waiting messages from the source (up to 10k)
        while (delayed.size() < 10000) {
            T event = source.next(systemCurrentTimeMillis);
            if (event == null) {
                break;
            }
            transformAndQueue(event, systemCurrentTimeMillis);
        }

        return delayed.nextReady(systemCurrentTimeMillis);
    }

    /**
     * Ignore any scheduled delays and return events in
     * schedule order until empty.
     */
    @Override
    public T drain() {
        T event = delayed.drain();
        if (event != null) {
            return event;
        }

        return source.drain();
    }
}
